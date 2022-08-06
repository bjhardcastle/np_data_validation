"""Strategies for looking up DataValidationFiles in a database and performing some action depending on context."""


from typing import List

import data_validation as dv


def exchange_if_checksum_in_db(subject: dv.DataValidationFile, matches: List[dv.DataValidationFile]) -> dv.DataValidationFile:
    """
    If the database has an entry for the subject file that already has a checksum, swap 
    the subject for the entry in the database. Saves us regenerating checksums for large files.
    """
    accepted_matches = [subject.Match.SELF, subject.Match.SELF_NO_CHECKSUM]
    # 


def delete_if_valid_backup_in_db(subject: dv.DataValidationFile, matches: List[dv.DataValidationFile]) -> None:
    """
    If the database has an entry for the subject file in known backup locations, or a new specified location, we can
    delete the subject.
    """
    accepted_matches = [subject.Match.VALID_COPY_RENAMED, subject.Match.VALID_COPY_SAME_NAME]
    #TODO check for accepted matches that contain backup locations in path
    #* this requires subject has checksum
    
def find_backup_if_not_in_db(subject: dv.DataValidationFile, matches: List[dv.DataValidationFile]) -> None:
    """
    If the database has no matches in backup locations we can go looking in lims/npexp/specified folder.
    """
    # accepted_matches >= [subject.Match.CHECKSUM_COLLISION]
    # a match could be anything here: but if it doesn't have a checksum we need to generate it 
    
    
def generate_checksum_if_not_in_db(subject: dv.DataValidationFile, db: dv.DataValidationDB) -> dv.DataValidationFile:
    """
    If the database has no entry for the subject file, generate a checksum for it.
    """
    accepted_matches = [subject.Match.SELF, subject.Match.SELF_NO_CHECKSUM]
    #TODO generate and add to db, after checking that there are no existing matches
    
    
    
def regenerate_checksums_on_mismatch(subject: dv.DataValidationFile, other: dv.DataValidationFile) -> None:
    """
    If the database has an entry for the subject file that has a different checksum, regenerate the checksum for it.
    """
    accepted_matches = [subject.Match.SELF, subject.Match.SELF_NO_CHECKSUM]
    #TODO regenerate and check again
    #* need a solution for replacing entries where the checksum is different

